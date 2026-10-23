use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::ScalarUDF;

use super::{NamedExpression, Resolver};
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::analysis::scope::ResolutionScope;
use crate::analysis::typing;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn validate_source_binding(
        &self,
        parts: &[String],
        plan_id: Option<i64>,
    ) -> PlanResult<()> {
        if let Some(schemas) = plan_id.and_then(|id| self.bindings.get(&id)) {
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

    pub(super) fn resolve_attribute(
        &mut self,
        id: ExpressionId,
        parts: &[String],
        plan_id: Option<i64>,
        schema: &PlanSchema,
    ) -> PlanResult<NamedExpression> {
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
            let mut value = NamedExpression {
                name: source.attribute(bound.id)?.name.clone(),
                expression: LogicalExpression {
                    id: if length == parts.len() {
                        id
                    } else {
                        self.context.ids.expression()
                    },
                    node: Deferred::Resolved(if bound.depth == 0 {
                        ExpressionNode::Attribute(bound.id)
                    } else {
                        ExpressionNode::OuterAttribute {
                            depth: bound.depth,
                            attribute: bound.id,
                        }
                    }),
                    result_type: Deferred::Resolved(bound.result_type),
                },
            };
            for (i, field) in parts[length..].iter().enumerate() {
                let extraction = NamedExpression {
                    name: field.clone(),
                    expression: LogicalExpression::literal(
                        self.context.ids.expression(),
                        ScalarValue::Utf8(Some(field.clone())),
                    ),
                };
                let selected_id = if i + length + 1 == parts.len() {
                    id
                } else {
                    self.context.ids.expression()
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
                let mut value = NamedExpression {
                    name: name.clone(),
                    expression: self.copy_expression(&expression)?,
                };
                for field in fields {
                    let extraction = NamedExpression {
                        name: field.clone(),
                        expression: self.literal(ScalarValue::Utf8(Some(field.clone()))),
                    };
                    let id = self.context.ids.expression();
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
        id: ExpressionId,
        parts: &[String],
        plan_id: Option<i64>,
    ) -> PlanResult<Option<NamedExpression>> {
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
                                if let Deferred::Resolved(ExpressionNode::Attribute(attribute)) =
                                    value.node
                                {
                                    value.node =
                                        Deferred::Resolved(ExpressionNode::OuterAttribute {
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

    pub(super) fn extract_value(
        &mut self,
        id: ExpressionId,
        child: NamedExpression,
        mut extraction: NamedExpression,
    ) -> PlanResult<NamedExpression> {
        let data_type = &child
            .expression
            .result_type
            .resolved("extraction input")?
            .data_type;
        let mut name = format!("{}[{}]", child.name, extraction.name);
        let function = match data_type {
            DataType::Struct(fields) => {
                name.clone_from(&extraction.name);
                let ExpressionNode::Literal(value) =
                    extraction.expression.node.resolved("struct selector")?
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
                extraction.expression.node = Deferred::Resolved(ExpressionNode::Literal(
                    ScalarValue::Utf8(Some(field.name().clone())),
                ));
                self.scalar_kernel("get_field")?
            }
            DataType::Map(_, _) => {
                let extracted = typing::resolve_scalar_call(
                    self.context.ids.expression(),
                    self.scalar_kernel("map_extract")?,
                    vec![child.expression, extraction.expression],
                    &mut self.context.ids,
                )?;
                let first = LogicalExpression::literal(
                    self.context.ids.expression(),
                    ScalarValue::Int64(Some(1)),
                );
                return Ok(NamedExpression {
                    name,
                    expression: typing::resolve_scalar_call(
                        id,
                        self.scalar_kernel("array_element")?,
                        vec![extracted, first],
                        &mut self.context.ids,
                    )?,
                });
            }
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
                if matches!(field.data_type(), DataType::Struct(_))
                    && matches!(
                        extraction.expression.node,
                        Deferred::Resolved(ExpressionNode::Literal(ScalarValue::Utf8(_)))
                    ) =>
            {
                let DataType::Struct(fields) = field.data_type() else {
                    unreachable!()
                };
                let key = crate::analysis::lowering::evaluate_constant(&extraction.expression)?;
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
                extraction.expression = self.literal(ScalarValue::Utf8(Some(field.name().clone())));
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
                let index = self.cast(extraction.expression, DataType::Int64)?;
                let index_copy = self.copy_expression(&index)?;
                let one = self.literal(ScalarValue::Int64(Some(1)));
                let position = self.binary(index_copy, datafusion_expr::Operator::Plus, one)?;
                let array_copy = self.copy_expression(&array)?;
                let element = self.scalar("array_element", vec![array_copy, position])?;
                let expression = if self.context.config.ansi_mode {
                    let array_copy = self.copy_expression(&array)?;
                    let length = self.scalar("array_length", vec![array_copy])?;
                    let length = self.cast(length, DataType::Int64)?;
                    let index_copy = self.copy_expression(&index)?;
                    let length_copy = self.copy_expression(&length)?;
                    let high =
                        self.binary(index_copy, datafusion_expr::Operator::GtEq, length_copy)?;
                    let index_copy = self.copy_expression(&index)?;
                    let zero = self.literal(ScalarValue::Int64(Some(0)));
                    let negative = self.binary(index_copy, datafusion_expr::Operator::Lt, zero)?;
                    let invalid = self.binary(high, datafusion_expr::Operator::Or, negative)?;
                    let present = self.is_not_null(array);
                    let invalid = self.binary(present, datafusion_expr::Operator::And, invalid)?;
                    let prefix = self.literal(ScalarValue::Utf8(Some(
                        "[INVALID_ARRAY_INDEX] The index ".into(),
                    )));
                    let index = self.cast(index, DataType::Utf8)?;
                    let middle = self.literal(ScalarValue::Utf8(Some(
                        " is out of bounds. The array has ".into(),
                    )));
                    let length = self.cast(length, DataType::Utf8)?;
                    let suffix = self.literal(ScalarValue::Utf8(Some(" elements. Use the SQL function `get()` to tolerate accessing element at invalid index and return NULL instead.".into())));
                    let message =
                        self.scalar("concat", vec![prefix, index, middle, length, suffix])?;
                    let error = self.scalar_udf(
                        Arc::new(ScalarUDF::from(
                            sail_function::scalar::misc::raise_error::RaiseError::new(),
                        )),
                        vec![message],
                    )?;
                    let error = self.cast(
                        error,
                        element
                            .result_type
                            .resolved("array element")?
                            .data_type
                            .clone(),
                    )?;
                    self.case(vec![(invalid, error)], Some(element))?
                } else {
                    element
                };
                return Ok(NamedExpression { name, expression });
            }
            _ => {
                return Err(PlanError::analysis(format!(
                    "cannot extract a field from {data_type}"
                )));
            }
        };
        Ok(NamedExpression {
            name,
            expression: typing::resolve_scalar_call(
                id,
                function,
                vec![child.expression, extraction.expression],
                &mut self.context.ids,
            )?,
        })
    }
}
