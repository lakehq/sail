use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common::spec;
use sail_common_datafusion::display::{ArrayFormatter, FormatOptions};
use sail_function::scalar::struct_function::StructFunction;

use super::{NamedExpression, Resolver};
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::analysis::typing;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    fn pivot_tuple(&mut self, mut values: Vec<LogicalExpression>) -> PlanResult<LogicalExpression> {
        if values.len() == 1 {
            return Ok(values.remove(0));
        }
        let names = (1..=values.len()).map(|i| format!("col{i}")).collect();
        self.scalar_udf(
            Arc::new(ScalarUDF::from(StructFunction::new(names))),
            values,
        )
    }

    fn pivot_predicate(
        &mut self,
        column: &Attribute,
        value: &ScalarValue,
        fast: bool,
    ) -> PlanResult<LogicalExpression> {
        let reference = self.reference(column)?;
        if value.is_null() {
            return Ok(LogicalExpression {
                id: self.context.ids.expression(),
                node: Deferred::Resolved(ExpressionNode::IsNull(Box::new(reference))),
                result_type: Deferred::Resolved(ExpressionType::new(DataType::Boolean, false)),
            });
        }
        let value = value.cast_to(&column.result_type.resolved("pivot column")?.data_type)?;
        if fast && scalar_is_nan(&value) {
            return Ok(self.literal(ScalarValue::Boolean(Some(false))));
        }
        let value = self.literal(value);
        self.binary(reference, Operator::Eq, value)
    }

    #[async_recursion::async_recursion]
    pub(super) async fn pivot(&mut self, pivot: spec::Pivot) -> PlanResult<LogicalPlan> {
        let input = self.query(*pivot.input).await?;
        let input = self.visible_output(input)?;
        let schema = input.schema.resolved("pivot input")?;
        let arity = pivot.columns.len();
        if arity == 0 {
            return Err(PlanError::analysis("pivot column required"));
        }
        let mut columns = vec![];
        for expression in pivot.columns {
            columns.push(self.expression(expression, schema).await?.expression);
        }
        let column_name = columns
            .iter()
            .map(|value| match &value.node {
                Deferred::Resolved(ExpressionNode::Attribute(id)) => schema
                    .attribute(*id)
                    .map(|a| a.name.clone())
                    .map_err(PlanError::from),
                _ => Ok("expression".to_string()),
            })
            .collect::<PlanResult<Vec<_>>>()?
            .join(", ");
        let column = self.pivot_tuple(columns)?;
        let mut excluded = HashSet::new();
        collect_attributes(&column, &mut excluded)?;
        let mut fast = true;
        for request in &pivot.aggregate {
            let expression = self.expression(request.clone(), schema).await?.expression;
            check_pivot_aggregate(&expression, schema)?;
            collect_attributes(&expression, &mut excluded)?;
            fast &= crate::analysis::typing::relational::pivot_first_supported(
                &expression
                    .result_type
                    .resolved("pivot aggregate")?
                    .data_type,
            );
        }
        let mut grouping = vec![];
        match pivot.grouping {
            Some(requests) => {
                for request in requests {
                    grouping.push(self.expression(request, schema).await?);
                }
            }
            None => {
                for attribute in &schema.attributes {
                    if !excluded.contains(&attribute.id) {
                        grouping.push(NamedExpression {
                            name: attribute.name.clone(),
                            expression: self.reference(attribute)?,
                        });
                    }
                }
            }
        }
        let (input, column) = self.append(input, column, "pivot_column".into())?;
        let mut values = vec![];
        if pivot.values.is_empty() {
            let reference = self.reference(&column)?;
            let distinct = self.project(
                input.clone(),
                vec![NamedExpression {
                    name: "pivot".into(),
                    expression: reference,
                }],
            )?;
            let distinct = LogicalPlan {
                schema: distinct.schema.clone(),
                node: Deferred::Resolved(PlanNode::Distinct {
                    input: Box::new(distinct),
                }),
            };
            let maximum = self.context.config.pivot_max_values;
            let distinct = self.limit(
                distinct,
                0,
                maximum
                    .saturating_add(1)
                    .try_into()
                    .map_err(|_| PlanError::invalid("pivot maximum is out of range"))?,
            );
            for batch in self.execute(distinct).await? {
                for row in 0..batch.num_rows() {
                    values.push((ScalarValue::try_from_array(batch.column(0), row)?, None));
                }
            }
            if values.len() > maximum {
                return Err(PlanError::analysis(format!(
                    "The pivot column {column_name} has more than {maximum} distinct values. Set spark.sql.pivotMaxValues to a larger value."
                )));
            }
            values.sort_by(|(a, _), (b, _)| {
                a.partial_cmp(b)
                    .unwrap_or_else(|| a.to_string().cmp(&b.to_string()))
            });
        } else {
            for value in pivot.values {
                if value.values.len() != arity {
                    return Err(PlanError::analysis(format!(
                        "pivot value has {} expressions, but pivot column has {arity}",
                        value.values.len()
                    )));
                }
                let mut expressions = vec![];
                for expression in value.values {
                    expressions.push(
                        self.expression(expression, &PlanSchema::default())
                            .await?
                            .expression,
                    );
                }
                let expression = self.pivot_tuple(expressions)?;
                values.push((
                    crate::analysis::lowering::evaluate_constant(&expression)?,
                    value.alias.map(String::from),
                ));
            }
        }
        let mut output = grouping
            .iter()
            .map(|e| NamedExpression {
                name: e.name.clone(),
                expression: e.expression.clone(),
            })
            .collect::<Vec<_>>();
        let single = pivot.aggregate.len() == 1;
        for (value, alias) in values {
            let value_name = match alias {
                Some(alias) => alias,
                None if value.is_null() => "null".into(),
                None => pivot_value_name(&value)?,
            };
            for request in &pivot.aggregate {
                let named = self
                    .expression(
                        request.clone(),
                        input.schema.resolved("pivot aggregate input")?,
                    )
                    .await?;
                let mut expression =
                    named
                        .expression
                        .transform_up::<PlanError>(&mut |mut expression| {
                            if let Deferred::Resolved(ExpressionNode::AggregateFunction {
                                function,
                                filter,
                                null_treatment,
                                ..
                            }) = &mut expression.node
                            {
                                let predicate = self.pivot_predicate(&column, &value, fast)?;
                                *filter = Some(Box::new(match filter.take() {
                                    Some(existing) => {
                                        self.binary(*existing, Operator::And, predicate)?
                                    }
                                    None => predicate,
                                }));
                                if !fast && matches!(function.name(), "first_value" | "last_value")
                                {
                                    *null_treatment =
                                        Some(datafusion_expr::expr::NullTreatment::IgnoreNulls);
                                }
                            }
                            Ok(expression)
                        })?;
                if fast {
                    let one = self.literal(ScalarValue::Int64(Some(1)));
                    let mut count =
                        self.aggregate_call(self.aggregate_kernel("count")?, vec![one])?;
                    let predicate = self.pivot_predicate(&column, &value, fast)?;
                    if let Deferred::Resolved(ExpressionNode::AggregateFunction {
                        filter, ..
                    }) = &mut count.node
                    {
                        *filter = Some(Box::new(predicate));
                    }
                    let zero = self.literal(ScalarValue::Int64(Some(0)));
                    let condition = self.binary(count, Operator::Gt, zero)?;
                    let null = self.literal(ScalarValue::Null);
                    expression = typing::resolve_case(
                        self.context.ids.expression(),
                        None,
                        vec![(condition, expression)],
                        Some(null),
                        &mut self.context.ids,
                    )?;
                }
                output.push(NamedExpression {
                    name: if single {
                        value_name.clone()
                    } else {
                        format!("{value_name}_{}", named.name)
                    },
                    expression,
                });
            }
        }
        self.aggregate(input, grouping, output, None)
    }

    #[async_recursion::async_recursion]
    pub(super) async fn unpivot(&mut self, unpivot: spec::Unpivot) -> PlanResult<LogicalPlan> {
        let input = self.query(*unpivot.input).await?;
        let input = self.visible_output(input)?;
        let schema = input.schema.resolved("unpivot input")?;
        let mut ids = vec![];
        let ids_specified = unpivot.ids.is_some();
        if let Some(requests) = unpivot.ids {
            for request in requests {
                ids.push(self.expression(request, schema).await?);
            }
        }
        let mut id_attributes = HashSet::new();
        for id in &ids {
            collect_attributes(&id.expression, &mut id_attributes)?;
        }
        let mut values = vec![];
        if let Some(requests) = unpivot.values {
            for request in requests {
                let mut columns = vec![];
                for column in request.columns {
                    columns.push(self.expression(column, schema).await?);
                }
                let name = request.alias.map(String::from).unwrap_or_else(|| {
                    columns
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join("_")
                });
                values.push((name, columns));
            }
        } else {
            if !ids_specified {
                return Err(PlanError::analysis(
                    "UNPIVOT requires value columns or ID columns",
                ));
            }
            for attribute in &schema.attributes {
                if !id_attributes.contains(&attribute.id) {
                    values.push((
                        attribute.name.clone(),
                        vec![NamedExpression {
                            name: attribute.name.clone(),
                            expression: self.reference(attribute)?,
                        }],
                    ));
                }
            }
        }
        if values.is_empty() {
            return Err(PlanError::analysis(
                "[UNPIVOT_REQUIRES_VALUE_COLUMNS] At least one value column needs to be specified for UNPIVOT",
            ));
        }
        let width = unpivot.value_column_names.len();
        if width == 0 || values.iter().any(|(_, values)| values.len() != width) {
            return Err(PlanError::analysis("UNPIVOT value column arity mismatch"));
        }
        if !ids_specified {
            let mut value_attributes = HashSet::new();
            for (_, columns) in &values {
                for column in columns {
                    collect_attributes(&column.expression, &mut value_attributes)?;
                }
            }
            for attribute in &schema.attributes {
                if !value_attributes.contains(&attribute.id) {
                    ids.push(NamedExpression {
                        name: attribute.name.clone(),
                        expression: self.reference(attribute)?,
                    });
                }
            }
        }
        let mut types = vec![];
        for index in 0..width {
            let fields = values
                .iter()
                .map(|(_, values)| {
                    Ok(values[index]
                        .expression
                        .result_type
                        .resolved("unpivot value")?
                        .data_type
                        .clone())
                })
                .collect::<PlanResult<Vec<_>>>()?;
            types.push(crate::analysis::typing::relational::unpivot_common_type(
                &fields,
            )?);
        }
        let variable: String = unpivot.variable_column_name.into();
        let names = std::iter::once(variable.clone())
            .chain(unpivot.value_column_names.into_iter().map(String::from))
            .collect::<Vec<_>>();
        let mut records = vec![];
        for (name, values) in values {
            let mut fields = vec![self.literal(ScalarValue::Utf8(Some(name)))];
            for (value, target) in values.into_iter().zip(&types) {
                fields.push(typing::coerce(
                    value.expression,
                    target,
                    &mut self.context.ids,
                )?);
            }
            records.push(self.scalar_udf(
                Arc::new(ScalarUDF::from(StructFunction::new(names.clone()))),
                fields,
            )?);
        }
        let array = self.scalar("make_array", records)?;
        let (input, array) = self.append(input, array, "unpivot_values".into())?;
        let (mut input, element) = self.unnest(input, &array, false)?;
        if !unpivot.include_nulls {
            let mut condition = self.literal(ScalarValue::Boolean(Some(false)));
            for name in &names[1..] {
                let reference = self.reference(&element)?;
                let selector = self.literal(ScalarValue::Utf8(Some(name.clone())));
                let value = self.scalar("get_field", vec![reference, selector])?;
                let present = LogicalExpression {
                    id: self.context.ids.expression(),
                    node: Deferred::Resolved(ExpressionNode::IsNotNull(Box::new(value))),
                    result_type: Deferred::Resolved(ExpressionType::new(DataType::Boolean, false)),
                };
                condition = self.binary(condition, Operator::Or, present)?;
            }
            input = Self::filter(input, condition)?;
        }
        for name in names {
            let reference = self.reference(&element)?;
            let selector = self.literal(ScalarValue::Utf8(Some(name.clone())));
            let expression = self.scalar("get_field", vec![reference, selector])?;
            ids.push(NamedExpression { name, expression });
        }
        let input = self.project(input, ids)?;
        Ok(LogicalPlan {
            schema: input.schema.clone(),
            node: Deferred::Resolved(PlanNode::Scope {
                input: Box::new(input),
            }),
        })
    }
}

fn collect_attributes(
    expression: &LogicalExpression,
    attributes: &mut HashSet<AttributeId>,
) -> PlanResult<()> {
    expression
        .clone()
        .transform_up::<PlanError>(&mut |expression| {
            if let Deferred::Resolved(ExpressionNode::Attribute(id)) = expression.node {
                attributes.insert(id);
            }
            Ok(expression)
        })?;
    Ok(())
}

fn check_pivot_aggregate(expression: &LogicalExpression, schema: &PlanSchema) -> PlanResult<()> {
    match expression.node.resolved("pivot aggregate")? {
        ExpressionNode::AggregateFunction { .. } => Ok(()),
        ExpressionNode::Attribute(id) => Err(PlanError::analysis(format!(
            "Aggregate expression required for pivot, but '{}' did not appear in any aggregate function",
            schema.attribute(*id)?.name
        ))),
        _ => {
            expression.clone().map_children::<PlanError>(&mut |child| {
                check_pivot_aggregate(&child, schema)?;
                Ok(child)
            })?;
            Ok(())
        }
    }
}

fn pivot_value_name(value: &ScalarValue) -> PlanResult<String> {
    let array = value.to_array()?;
    let formatter = ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default())?;
    Ok(formatter.value(0).try_to_string()?)
}

fn scalar_is_nan(value: &ScalarValue) -> bool {
    match value {
        ScalarValue::Float16(Some(x)) => x.is_nan(),
        ScalarValue::Float32(Some(x)) => x.is_nan(),
        ScalarValue::Float64(Some(x)) => x.is_nan(),
        _ => false,
    }
}
