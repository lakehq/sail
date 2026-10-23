use std::collections::HashSet;

use datafusion_common::ScalarValue;
use datafusion_expr::{JoinType, Operator, WindowFrame, WindowFunctionDefinition};
use sail_common::spec;

use super::{NamedExpression, Resolver};
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::analysis::typing;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    #[async_recursion::async_recursion]
    pub(super) async fn set_operation(
        &mut self,
        set: spec::SetOperation,
    ) -> PlanResult<LogicalPlan> {
        if set.allow_missing_columns && !set.by_name {
            return Err(PlanError::analysis("missing columns require UNION BY NAME"));
        }
        if set.by_name && set.set_op_type != spec::SetOpType::Union {
            return Err(PlanError::analysis("BY NAME is only supported for UNION"));
        }
        let left = self.query(*set.left).await?;
        let right = self.query(*set.right).await?;
        let visible = |plan: &LogicalPlan| -> PlanResult<Vec<Attribute>> {
            Ok(plan
                .schema
                .resolved("set operation input")?
                .attributes
                .iter()
                .filter(|a| !a.hidden)
                .cloned()
                .collect())
        };
        let a = visible(&left)?;
        let b = visible(&right)?;
        let pairs = if set.by_name {
            for columns in [&a, &b] {
                let mut names = HashSet::new();
                for column in columns {
                    if !names.insert(self.name_key(&column.name)) {
                        return Err(PlanError::analysis(
                            "UNION BY NAME has duplicate input names",
                        ));
                    }
                }
            }
            let mut pairs = vec![];
            for column in &a {
                let other = b
                    .iter()
                    .find(|other| self.name_key(&column.name) == self.name_key(&other.name));
                if other.is_none() && !set.allow_missing_columns {
                    return Err(PlanError::analysis(format!(
                        "UNION BY NAME is missing {}",
                        column.name
                    )));
                }
                pairs.push((Some(column.clone()), other.cloned()));
            }
            for column in &b {
                if !a
                    .iter()
                    .any(|other| self.name_key(&column.name) == self.name_key(&other.name))
                {
                    if !set.allow_missing_columns {
                        return Err(PlanError::analysis(format!(
                            "UNION BY NAME is missing {}",
                            column.name
                        )));
                    }
                    pairs.push((None, Some(column.clone())));
                }
            }
            pairs
        } else {
            if a.len() != b.len() {
                return Err(PlanError::analysis(
                    "set operation inputs have different widths",
                ));
            }
            a.into_iter()
                .zip(b)
                .map(|(a, b)| (Some(a), Some(b)))
                .collect()
        };
        let mut outputs = vec![];
        let mut left_values = vec![];
        let mut right_values = vec![];
        for (a, b) in pairs {
            let name = a
                .as_ref()
                .or(b.as_ref())
                .ok_or_else(|| PlanError::internal("missing set operation column"))?
                .name
                .clone();
            let mut values = vec![];
            for attribute in [&a, &b] {
                values.push(match attribute {
                    Some(attribute) => self.reference(attribute)?,
                    None => {
                        LogicalExpression::literal(self.context.ids.expression(), ScalarValue::Null)
                    }
                });
            }
            let fields = typing::argument_fields(&values)?;
            let common = sail_type_system::coercion::common_type(
                &fields
                    .iter()
                    .map(|f| f.data_type().clone())
                    .collect::<Vec<_>>(),
            )?;
            let mut values = values
                .into_iter()
                .map(|value| typing::coerce(value, &common, &mut self.context.ids))
                .collect::<PlanResult<Vec<_>>>()?
                .into_iter();
            let mut left_value = values
                .next()
                .ok_or_else(|| PlanError::internal("missing left set value"))?;
            let mut right_value = values
                .next()
                .ok_or_else(|| PlanError::internal("missing right set value"))?;
            let mut result_type = left_value
                .result_type
                .resolved("set operation type")?
                .clone();
            let other_type = right_value.result_type.resolved("set operation type")?;
            if a.is_none() {
                result_type.metadata.clone_from(&other_type.metadata);
            } else if b.is_some() {
                // User field metadata does not constrain SQL set compatibility.
                result_type
                    .metadata
                    .retain(|key, value| other_type.metadata.get(key) == Some(value));
            }
            result_type.nullable |= other_type.nullable;
            for value in [&mut left_value, &mut right_value] {
                let mut field = value.result_type.resolved("set input field")?.clone();
                if field.metadata != result_type.metadata {
                    field.metadata.clone_from(&result_type.metadata);
                    *value = LogicalExpression {
                        id: self.context.ids.expression(),
                        result_type: Deferred::Resolved(field),
                        node: Deferred::Resolved(ExpressionNode::WithMetadata(Box::new(
                            value.clone(),
                        ))),
                    };
                }
            }
            let mut output = self.attribute(name.clone(), result_type);
            if let Some(source) = &a {
                output.origin = source.origin;
            }
            outputs.push(output);
            left_values.push(NamedExpression {
                name: name.clone(),
                expression: left_value,
            });
            right_values.push(NamedExpression {
                name,
                expression: right_value,
            });
        }
        let mut left = self.project(left, left_values)?;
        let mut right = self.project(right, right_values)?;
        if set.set_op_type == spec::SetOpType::Union {
            let union = LogicalPlan {
                node: Deferred::Resolved(PlanNode::Union {
                    inputs: vec![left, right],
                }),
                schema: Deferred::Resolved(PlanSchema {
                    attributes: outputs,
                }),
            };
            return Ok(if set.is_all {
                union
            } else {
                Self::distinct(union)
            });
        }
        let width = left.schema.resolved("set input")?.attributes.len();
        if set.is_all {
            left = self.number_duplicates(left)?;
            right = self.number_duplicates(right)?;
        } else {
            left = Self::distinct(left);
        }
        let mut condition = None;
        for (a, b) in left
            .schema
            .resolved("set left")?
            .attributes
            .iter()
            .zip(&right.schema.resolved("set right")?.attributes)
        {
            let a = self.reference(a)?;
            let b = self.reference(b)?;
            let equals = typing::resolve_binary(
                self.context.ids.expression(),
                a,
                Operator::IsNotDistinctFrom,
                b,
                &mut self.context.ids,
            )?;
            condition = Some(match condition {
                None => equals,
                Some(previous) => typing::resolve_binary(
                    self.context.ids.expression(),
                    previous,
                    Operator::And,
                    equals,
                    &mut self.context.ids,
                )?,
            });
        }
        let schema = left.schema.clone();
        let join = LogicalPlan {
            node: Deferred::Resolved(PlanNode::Join {
                lateral: false,
                left: Box::new(left),
                right: Box::new(right),
                kind: if set.set_op_type == spec::SetOpType::Intersect {
                    JoinType::LeftSemi
                } else {
                    JoinType::LeftAnti
                },
                condition: condition.map(Box::new),
            }),
            schema,
        };
        let output = join
            .schema
            .resolved("set output")?
            .attributes
            .iter()
            .take(width)
            .map(|a| {
                Ok(NamedExpression {
                    name: a.name.clone(),
                    expression: self.reference(a)?,
                })
            })
            .collect::<PlanResult<_>>()?;
        self.project(join, output)
    }

    fn distinct(input: LogicalPlan) -> LogicalPlan {
        let schema = input.schema.clone();
        LogicalPlan {
            node: Deferred::Resolved(PlanNode::Distinct {
                input: Box::new(input),
            }),
            schema,
        }
    }

    fn number_duplicates(&mut self, input: LogicalPlan) -> PlanResult<LogicalPlan> {
        let mut schema = input.schema.resolved("duplicate input")?.clone();
        let partition_by = schema
            .attributes
            .iter()
            .map(|a| self.reference(a))
            .collect::<PlanResult<_>>()?;
        let function = WindowFunctionDefinition::WindowUDF(
            datafusion::functions_window::row_number::row_number_udwf(),
        );
        let field = sail_type_system::aggregate::resolve_window(&function, &[])?.result;
        let result_type = ExpressionType::from_field(&field);
        let expression = LogicalExpression {
            id: self.context.ids.expression(),
            node: Deferred::Resolved(ExpressionNode::WindowFunction {
                function,
                arguments: vec![],
                partition_by,
                order_by: vec![],
                frame: WindowFrame::new(None),
                filter: None,
                distinct: false,
                null_treatment: None,
            }),
            result_type: Deferred::Resolved(result_type.clone()),
        };
        let attribute = self.attribute("duplicate_number".into(), result_type);
        schema.attributes.push(attribute.clone());
        Ok(LogicalPlan {
            node: Deferred::Resolved(PlanNode::Window {
                input: Box::new(input),
                expressions: vec![Projection {
                    expression,
                    attribute,
                }],
            }),
            schema: Deferred::Resolved(schema),
        })
    }
}
